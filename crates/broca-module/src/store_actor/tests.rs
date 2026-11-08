use std::sync::Arc;

use async_trait::async_trait;
use broca_core::provider::StubProvider;
use broca_core::stub_plane::StubToolPlane;
use broca_core::{OutputEvent, RunError, RunSummary};
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason};
use broca_protocol::request::StopCondition;
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, SubmissionRequest, Usage,
};
use broca_session_loop::parts::{MemoryParts, SharedParts};
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
use crate::executor::{
    RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
};

pub(super) fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
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
pub(super) fn read_step(id: &str) -> Vec<NormalizedEvent> {
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

pub(super) fn bind() -> BindIdentity {
    BindIdentity::new("/project", "store-actor", "s1")
}

/// A scripted provider whose first step waits for `gate`, so a test can act while a run's start
/// is durable and its first model call is in flight.
pub(super) struct Gated {
    pub(super) inner: StubProvider,
    pub(super) gate: Option<Arc<Notify>>,
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
pub(super) struct LoopExecutor {
    pub(super) provider: Gated,
    /// When set, a run drained from the queue signals the first `Notify` and waits on the second
    /// before its admission, so a test can act while the drain has started a run whose start
    /// isn't durable yet.
    drain_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    /// The planes the executor offers the session's runs.
    pub(super) planes: Planes,
    /// When set, each tool call's approval signals the first `Notify` and waits on the second,
    /// so a test can act after a step that called tools was appended and before its batch's
    /// intents make it durable.
    pub(super) approval_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    /// What the `read` tool answers.
    pub(super) tool_text: String,
}

/// Allows every call, once the test lets it (see [`LoopExecutor::approval_gate`]).
struct GatedApproval(Option<(Arc<Notify>, Arc<Notify>)>);

#[async_trait]
impl broca_core::capability::ApprovalPolicy for GatedApproval {
    async fn decide(&self, call: &broca_protocol::ToolCall) -> broca_protocol::ApprovalResponse {
        if let Some((reached, release)) = &self.0 {
            reached.notify_one();
            release.notified().await;
        }
        broca_protocol::ApprovalResponse::Allow {
            tool_call_id: call.tool_call_id.clone(),
        }
    }
}

/// The hook and compaction planes a test executor offers, standing in for the ones the module's
/// executor reaches over the daemon; none by default.
#[derive(Clone, Default)]
pub(super) struct Planes {
    pub(super) hooks: Option<Arc<dyn broca_core::HookPlane>>,
    pub(super) compaction: Option<Arc<dyn broca_core::CompactionPlane>>,
}

pub(super) fn executor(
    steps: Vec<Vec<NormalizedEvent>>,
    gate: Option<Arc<Notify>>,
) -> LoopExecutor {
    LoopExecutor {
        provider: Gated {
            inner: StubProvider::new(steps),
            gate,
        },
        drain_gate: None,
        planes: Planes::default(),
        approval_gate: None,
        tool_text: "file contents".into(),
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

    fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
        self.planes.hooks.clone()
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        self.planes.compaction.clone()
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        let StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            mode,
            ..
        } = job;
        // The module's executor admits a run's context limit from the caller's override when
        // the send names one.
        control.admitted_context_limit = control.admitted_context_limit.or(req.context_limit);
        if let (Some((reached, release)), Some(_)) = (&self.drain_gate, &req.submission_id) {
            reached.notify_one();
            release.notified().await;
        }
        let result = async {
            let admission = if mode != StoreRunMode::Fresh {
                None
            } else {
                Some(
                    broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &self.provider,
                        writer.operations(),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?,
                )
            };
            let tools = StubToolPlane::new().with_canned_text("read", &self.tool_text);
            let now = || 1;
            let approval = GatedApproval(self.approval_gate.clone());
            let seams = Seams {
                provider: &self.provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &approval,
                hook_plane: control.hook_plane.clone(),
                now_ms: &now,
                session_address: None,
                compaction_plane: control.compaction_plane.as_deref(),
                compaction_session: control.compaction_session.clone(),
                // The module's executor has every store run tell the compaction provider of
                // the session's changes outside its steps (`Seams::compaction_changes`).
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: control.step_progress.clone(),
            };
            // The test's credential is always ready, so a sign-in pause resumes.
            match (admission, mode) {
                (Some(admission), _) => run(&mut writer, parts.as_mut(), admission, &seams).await,
                (None, StoreRunMode::Seal(cause)) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
                }
                (None, _) => {
                    broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await
                }
            }
            .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

/// A store actor over a new session in memory, serving on its own task, and the backend it
/// writes to.
pub(super) async fn actor(
    executor: LoopExecutor,
) -> (
    mpsc::UnboundedSender<LineageCmd>,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
    &'static MemoryBackend,
) {
    actor_with(executor, SharedParts::new(Box::new(MemoryParts::default()))).await
}

/// [`actor`] over the setup parts store `parts`, which a reopened actor can share.
pub(super) async fn actor_with(
    executor: LoopExecutor,
    parts: SharedParts,
) -> (
    mpsc::UnboundedSender<LineageCmd>,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
    &'static MemoryBackend,
) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = StoreActor::new(bind(), session, Arc::new(executor), Box::new(parts));
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    (commands, served, backend)
}

pub(super) const ADDR: &str = "0123456789abcdef";

pub(super) fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "store-actor".into(),
        session: "s1".into(),
    }
}

pub(super) fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[7; 32])
}

/// A store actor over the session `backend` already holds, loaded as a module opening it after
/// a restart loads it.
pub(super) async fn reopen(
    backend: &'static MemoryBackend,
    executor: Arc<LoopExecutor>,
    parts: &SharedParts,
) -> (
    mpsc::UnboundedSender<LineageCmd>,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
) {
    let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let actor = StoreActor::new(bind(), session, executor, Box::new(parts.clone()));
    let (commands, receiver) = mpsc::unbounded_channel();
    (commands, tokio::spawn(actor.serve(receiver)))
}

pub(super) fn request(text: &str) -> SubmissionRequest {
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

pub(super) async fn send(
    commands: &mpsc::UnboundedSender<LineageCmd>,
    run_id: &str,
    text: &str,
) -> SendOutcome {
    send_with(commands, run_id, request(text), SendKey::Minted).await
}

pub(super) async fn send_with(
    commands: &mpsc::UnboundedSender<LineageCmd>,
    run_id: &str,
    request: SubmissionRequest,
    key: SendKey,
) -> SendOutcome {
    send_as(
        commands,
        run_id,
        request,
        key,
        broca_protocol::Delivery::Queue,
    )
    .await
}

/// A send with the delivery mode `delivery`.
pub(super) async fn send_as(
    commands: &mpsc::UnboundedSender<LineageCmd>,
    run_id: &str,
    request: SubmissionRequest,
    key: SendKey,
    delivery: broca_protocol::Delivery,
) -> SendOutcome {
    send_labelled(commands, run_id, request, key, delivery, None).await
}

/// A send with the delivery mode `delivery`, naming the session label `session_class`.
pub(super) async fn send_labelled(
    commands: &mpsc::UnboundedSender<LineageCmd>,
    run_id: &str,
    request: SubmissionRequest,
    key: SendKey,
    delivery: broca_protocol::Delivery,
    session_class: Option<&str>,
) -> SendOutcome {
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Send {
            session_class: session_class.map(str::to_owned),
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId(run_id.into()),
            request: Box::new(request),
            kind: RunKind::FreshOrResume,
            key,
            skip_materialization: false,
            delivery,
            plan: None,
            order: None,
            reply,
        })
        .unwrap();
    answer.await.unwrap()
}

/// A send keyed by the caller's `send_id` `k`, whose run id is `r-k`.
async fn keyed(commands: &mpsc::UnboundedSender<LineageCmd>, k: &str, text: &str) -> SendOutcome {
    send_with(
        commands,
        &format!("r-{k}"),
        request(text),
        SendKey::SendId(k.into()),
    )
    .await
}

/// How many runs the session's open log started.
async fn starts(session: &Session<&'static MemoryBackend>) -> usize {
    session
        .open_log_records()
        .await
        .unwrap()
        .iter()
        .filter(|(_, record)| matches!(record, Record::RunStart(_)))
        .count()
}

fn finished(run_id: &str) -> SendOutcome {
    SendOutcome::Finished {
        run_id: RunId(run_id.into()),
        reason: broca_protocol::RunFinishReason::Completed,
        delivered: None,
    }
}

/// A provider step that fails because the credential needs signing in again.
pub(super) fn auth_failure() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(ProviderControl::Error {
        error: serde_json::from_value(serde_json::json!({
            "class": "auth",
            "message": "sign in again",
            "status": 401
        }))
        .unwrap(),
        retries_used: 0,
    })]
}

async fn status(commands: &mpsc::UnboundedSender<LineageCmd>) -> LineageStatus {
    let (reply, answer) = oneshot::channel();
    commands.send(LineageCmd::Status { reply }).unwrap();
    answer.await.unwrap()
}

/// Waits until the session is idle again.
pub(super) async fn idle(commands: &mpsc::UnboundedSender<LineageCmd>) {
    for _ in 0..500 {
        if status(commands).await == LineageStatus::Idle {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("the session never went idle");
}

pub(super) fn texts(session: &Session<&'static MemoryBackend>) -> Vec<String> {
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
        .send(LineageCmd::RepairRetention { reply })
        .unwrap();
    assert_eq!(
        answer.await.unwrap(),
        Err("retention repair is not served yet for a session kept in the new storage".into())
    );
}

/// A keyed send's run records its key and its payload's identity digest. A retry with the same
/// payload is answered from that run, here finished, and writes nothing.
#[tokio::test]
async fn a_keyed_retry_is_answered_from_the_run_it_started() {
    let (commands, served, _) = actor(executor(vec![text_step("a1", "hello")], None)).await;
    assert!(matches!(
        keyed(&commands, "k1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert_eq!(keyed(&commands, "k1", "first").await, finished("r-k1"));
    drop(commands);
    let session = served.await.unwrap();
    let start = session
        .operations()
        .run
        .as_ref()
        .unwrap()
        .start
        .clone()
        .unwrap();
    assert_eq!(start.send_id.as_deref(), Some("k1"));
    assert_eq!(
        start.request_identity_digest,
        Some(broca_core::send_identity_digest_with_conversation_key(
            &request("first"),
            broca_protocol::Delivery::Queue,
            None,
        ))
    );
    assert_eq!(texts(&session), ["first", "hello"], "the retry ran nothing");
    assert_eq!(starts(&session).await, 1);
}

/// A retry whose payload differs from the send its key started is refused naming the field that
/// differs, and writes nothing.
#[tokio::test]
async fn a_retry_with_another_payload_is_refused_naming_what_differs() {
    let (commands, served, _) = actor(executor(vec![text_step("a1", "hello")], None)).await;
    assert!(matches!(
        keyed(&commands, "k1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert_eq!(
        keyed(&commands, "k1", "changed").await,
        SendOutcome::SendIdReuse { differing: "input" }
    );
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), ["first", "hello"]);
    assert_eq!(
        starts(&session).await,
        1,
        "the refused retry started nothing"
    );
}

/// A retry of the run the session is running is answered `active`, and once that run ends, with
/// how it ended.
#[tokio::test]
async fn a_retry_of_the_running_run_is_active_until_it_ends() {
    let gate = Arc::new(Notify::new());
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello")],
        Some(Arc::clone(&gate)),
    ))
    .await;
    let active = SendOutcome::Active {
        run_id: RunId("r-k1".into()),
        delivered: None,
    };
    assert_eq!(keyed(&commands, "k1", "first").await, active);
    assert_eq!(keyed(&commands, "k1", "first").await, active);
    gate.notify_one();
    idle(&commands).await;
    assert_eq!(keyed(&commands, "k1", "first").await, finished("r-k1"));
    drop(commands);
    assert_eq!(texts(&served.await.unwrap()), ["first", "hello"]);
}

/// A retry of a run that is no longer the session's latest is answered from that run's own end,
/// found by its run id.
#[tokio::test]
async fn a_retry_of_an_earlier_run_is_answered_from_its_end() {
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        None,
    ))
    .await;
    assert!(matches!(
        keyed(&commands, "k1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert!(matches!(
        send(&commands, "r2", "second").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert_eq!(keyed(&commands, "k1", "first").await, finished("r-k1"));
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), ["first", "hello", "second", "again"]);
}

/// A run paused for sign-in owns the session: a new send is refused naming it and starts
/// nothing. A retry of its own send resumes it as the same run, which then answers, and no
/// second run starts.
#[tokio::test]
async fn a_sign_in_pause_resumes_on_its_retry() {
    let (commands, served, _) = actor(executor(
        vec![auth_failure(), text_step("a1", "hello")],
        None,
    ))
    .await;
    assert!(matches!(
        keyed(&commands, "k1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert_eq!(
        send(&commands, "r2", "second").await,
        SendOutcome::RunPaused {
            run_id: RunId("r-k1".into()),
            reason: broca_protocol::PauseReason::AuthRequired,
        }
    );
    assert_eq!(
        keyed(&commands, "k1", "first").await,
        SendOutcome::Active {
            run_id: RunId("r-k1".into()),
            delivered: None
        }
    );
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let run = session.operations().run.as_ref().unwrap();
    assert_eq!(run.run_id, "r-k1");
    assert_eq!(run.paused, None);
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert_eq!(texts(&session), ["first", "hello"]);
    assert_eq!(
        starts(&session).await,
        1,
        "the resume started no second run"
    );
}

/// While a run is running, a send naming another routing decision is refused as today's actor
/// refuses it, before its key is looked at.
#[tokio::test]
async fn a_running_runs_routing_decision_is_fixed() {
    let gate = Arc::new(Notify::new());
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello")],
        Some(Arc::clone(&gate)),
    ))
    .await;
    let mut first = request("first");
    first.route_decision_id = Some("decision-1".into());
    assert!(matches!(
        send_with(&commands, "r-k1", first, SendKey::SendId("k1".into())).await,
        SendOutcome::Active { .. }
    ));
    let mut other = request("first");
    other.route_decision_id = Some("decision-2".into());
    assert_eq!(
        send_with(&commands, "r-k1", other, SendKey::SendId("k1".into())).await,
        SendOutcome::RouteDecisionFixed
    );
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    assert_eq!(texts(&served.await.unwrap()), ["first", "hello"]);
}

/// A send while a run runs is queued durably, answered `pending`, and starts when that run ends,
/// consuming its queued send. The actor starts the drained run before it reads its next command,
/// so the session reads idle only once both runs ended.
#[tokio::test]
async fn a_queued_send_runs_when_the_run_before_it_ends() {
    let gate = Arc::new(Notify::new());
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        Some(Arc::clone(&gate)),
    ))
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    assert_eq!(
        send(&commands, "r2", "second").await,
        SendOutcome::Pending {
            submission_id: broca_protocol::SubmissionId("sub-r2".into())
        }
    );
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), ["first", "hello", "second", "again"]);
    let start = session
        .operations()
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
        .unwrap();
    assert_eq!(start.run_id, "r2");
    assert_eq!(start.submission_id.as_deref(), Some("sub-r2"));
    assert!(session.operations().queue.is_empty());
}

/// A queued send whose run admission refuses ends with a retraction carrying the refusal, and
/// the send behind it still runs.
#[tokio::test]
async fn a_refused_queued_send_ends_and_the_next_one_runs() {
    let gate = Arc::new(Notify::new());
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        Some(Arc::clone(&gate)),
    ))
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    let mut refused = request("refused");
    refused.stop_when = vec![StopCondition::StepCount { n: 0 }];
    assert!(matches!(
        send_with(&commands, "r2", refused, SendKey::Minted).await,
        SendOutcome::Pending { .. }
    ));
    assert!(matches!(
        send(&commands, "r3", "third").await,
        SendOutcome::Pending { .. }
    ));
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), ["first", "hello", "third", "again"]);
    let records = session.open_log_records().await.unwrap();
    let failure = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::Retraction(retraction) if retraction.submission_id == "sub-r2" => {
                retraction.failure.clone()
            }
            _ => None,
        })
        .expect("the refused send's retraction carries its refusal");
    assert_eq!(failure["run_id"], "r2");
    assert!(session.operations().queue.is_empty());
}

/// A steer while a run runs joins it: it is answered `active` naming that run, and the run places
/// it at its next step boundary, taking one more step to answer it.
#[tokio::test]
async fn a_steer_joins_the_running_run() {
    let gate = Arc::new(Notify::new());
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        Some(Arc::clone(&gate)),
    ))
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    assert_eq!(
        send_as(
            &commands,
            "s1",
            request("steered"),
            SendKey::Minted,
            broca_protocol::Delivery::Steer
        )
        .await,
        SendOutcome::Active {
            run_id: RunId("r1".into()),
            delivered: None
        }
    );
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), ["first", "hello", "steered", "again"]);
    assert_eq!(starts(&session).await, 1, "the steer joined the run");
    let run = session.operations().run.as_ref().unwrap();
    assert_eq!(run.completed_steps, 2);
}

/// A steer naming a setting the running run froze otherwise is refused naming it, and writes
/// nothing.
#[tokio::test]
async fn a_steer_with_another_model_is_refused_naming_it() {
    let gate = Arc::new(Notify::new());
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello")],
        Some(Arc::clone(&gate)),
    ))
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    let mut other = request("steered");
    other.model = broca_protocol::request::ModelHandle::new("stub", "another");
    assert_eq!(
        send_as(
            &commands,
            "s1",
            other,
            SendKey::Minted,
            broca_protocol::Delivery::Steer
        )
        .await,
        SendOutcome::SteerSettingsConflict { field: "model" }
    );
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), ["first", "hello"]);
    assert!(!session
        .open_log_records()
        .await
        .unwrap()
        .iter()
        .any(|(_, record)| matches!(record, Record::Steer(_))));
}

pub(super) async fn retract(
    commands: &mpsc::UnboundedSender<LineageCmd>,
    submission_id: &str,
) -> crate::actor::RetractOutcome {
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Retract {
            submission_id: broca_protocol::SubmissionId(submission_id.into()),
            reply,
        })
        .unwrap();
    answer.await.unwrap()
}

/// A queued send can be retracted before it runs: its retraction is durable, a second one finds
/// nothing pending, and the send never runs. Once a send's run started, retracting it is too
/// late.
#[tokio::test]
async fn a_queued_send_is_retracted_before_it_runs() {
    use crate::actor::RetractOutcome;
    let gate = Arc::new(Notify::new());
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        Some(Arc::clone(&gate)),
    ))
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    for (run, text) in [("r2", "withdrawn"), ("r3", "third")] {
        assert!(matches!(
            send(&commands, run, text).await,
            SendOutcome::Pending { .. }
        ));
    }
    assert_eq!(
        retract(&commands, "sub-r2").await,
        RetractOutcome::Retracted
    );
    assert_eq!(
        retract(&commands, "sub-r2").await,
        RetractOutcome::NotPending
    );
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    assert_eq!(
        retract(&commands, "sub-r3").await,
        RetractOutcome::AlreadyStarted
    );
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), ["first", "hello", "third", "again"]);
    let records = session.open_log_records().await.unwrap();
    assert!(records.iter().any(|(_, record)| matches!(
        record,
        Record::Retraction(retraction)
            if retraction.submission_id == "sub-r2" && retraction.failure.is_none()
    )));
}

/// Commands wait until a drained run's start is durable, as they wait for a sender's own run:
/// a retraction sent while the drain has started the queued send's run, but before that run's
/// start is written, is answered after the start, as too late. Answered earlier, it would write
/// a retraction for a send whose run then starts anyway.
#[tokio::test]
async fn a_retraction_waits_for_a_drained_runs_start() {
    use crate::actor::RetractOutcome;
    let gate = Arc::new(Notify::new());
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut gated = executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        Some(Arc::clone(&gate)),
    );
    gated.drain_gate = Some((Arc::clone(&reached), Arc::clone(&release)));
    let (commands, served, _) = actor(gated).await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        send(&commands, "r2", "queued").await,
        SendOutcome::Pending { .. }
    ));
    gate.notify_one();
    reached.notified().await;
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Retract {
            submission_id: broca_protocol::SubmissionId("sub-r2".into()),
            reply,
        })
        .unwrap();
    // Time for an actor that took commands now to answer, before the run may write its start.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    release.notify_one();
    gate.notify_one();
    assert_eq!(answer.await.unwrap(), RetractOutcome::AlreadyStarted);
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), ["first", "hello", "queued", "again"]);
}

/// A process that crashed while `r1` ran (its first model call in flight) and with `r2` queued
/// behind it: the actor's task is dropped without ending the run, as a killed process leaves
/// it. Returns the backend holding the session and its setup parts store.
async fn crashed_with_a_queued_send() -> (&'static MemoryBackend, SharedParts) {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let never = Arc::new(Notify::new());
    let (commands, served, backend) =
        actor_with(executor(Vec::new(), Some(never)), parts.clone()).await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        send(&commands, "r2", "second").await,
        SendOutcome::Pending { .. }
    ));
    served.abort();
    let _ = served.await;
    (backend, parts)
}

/// The records a reopened session appended, from the writer's barrier on.
async fn appended_after_reopen(session: &Session<&'static MemoryBackend>) -> Vec<Record> {
    let records = session.open_log_records().await.unwrap();
    let opened = records
        .iter()
        .rposition(|(_, record)| matches!(record, Record::WriterOpened(_)))
        .expect("the reopened writer's barrier");
    records[opened + 1..]
        .iter()
        .map(|(_, record)| record.clone())
        .collect()
}

/// A run a crashed process left running is ended `Interrupted` when the session opens, as
/// today's startup pass seals it: no model call, the crash named on the run's end. The send
/// queued behind it then runs.
#[tokio::test]
async fn a_crashed_run_ends_interrupted_on_open_and_the_queue_drains() {
    let (backend, parts) = crashed_with_a_queued_send().await;
    let executor = Arc::new(executor(vec![text_step("a2", "again")], None));
    let (commands, served) = reopen(backend, Arc::clone(&executor), &parts).await;
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let ends: Vec<_> = appended_after_reopen(&session)
        .await
        .into_iter()
        .filter_map(|record| match record {
            Record::RunEnd(end) => Some(end),
            _ => None,
        })
        .collect();
    assert_eq!(ends.len(), 2, "{ends:?}");
    assert_eq!(ends[0].run_id, "r1");
    assert_eq!(ends[0].reason, broca_protocol::RunFinishReason::Interrupted);
    assert_eq!(ends[0].interrupt_reason.as_deref(), Some("runner_crash"));
    assert_eq!(ends[1].run_id, "r2");
    assert_eq!(ends[1].reason, broca_protocol::RunFinishReason::Completed);
    assert_eq!(
        executor.provider.inner.calls().len(),
        1,
        "only r2 called the model"
    );
    assert_eq!(texts(&session), ["first", "second", "again"]);
}

/// A run left running with a boundary cancel pending resumes when the session opens, so the
/// cancel ends it at its next boundary, as today's recovery carries out a pending cancel. The
/// send queued behind it then runs.
#[tokio::test]
async fn a_running_run_with_a_cancel_pending_resumes_to_its_cancel_on_open() {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let never = Arc::new(Notify::new());
    let (commands, served, backend) =
        actor_with(executor(Vec::new(), Some(never)), parts.clone()).await;
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
    assert!(matches!(
        send(&commands, "r2", "second").await,
        SendOutcome::Pending { .. }
    ));
    served.abort();
    let _ = served.await;
    let executor = Arc::new(executor(
        vec![read_step("a1"), text_step("a2", "again")],
        None,
    ));
    let (commands, served) = reopen(backend, executor, &parts).await;
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let ends: Vec<_> = appended_after_reopen(&session)
        .await
        .into_iter()
        .filter_map(|record| match record {
            Record::RunEnd(end) => Some((end.run_id, end.reason)),
            _ => None,
        })
        .collect();
    assert_eq!(
        ends,
        [
            ("r1".to_owned(), broca_protocol::RunFinishReason::Cancelled),
            ("r2".to_owned(), broca_protocol::RunFinishReason::Completed),
        ]
    );
}

/// A run paused for sign-in stays paused when the session opens: nothing is written for it,
/// and its retry still resumes it.
#[tokio::test]
async fn a_sign_in_pause_stays_paused_on_open() {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let (commands, served, backend) =
        actor_with(executor(vec![auth_failure()], None), parts.clone()).await;
    assert!(matches!(
        keyed(&commands, "k1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    drop(served.await.unwrap());
    let executor = Arc::new(executor(vec![text_step("a1", "hello")], None));
    let (commands, served) = reopen(backend, Arc::clone(&executor), &parts).await;
    assert_eq!(
        send(&commands, "r2", "second").await,
        SendOutcome::RunPaused {
            run_id: RunId("r-k1".into()),
            reason: broca_protocol::PauseReason::AuthRequired,
        }
    );
    assert!(executor.provider.inner.calls().is_empty());
    assert!(matches!(
        keyed(&commands, "k1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert_eq!(texts(&session), ["first", "hello"]);
}

/// A process that crashed after `r1` ended, while the drain was starting the run of `r2`,
/// which was queued behind it, before that run's start was written. Opening the session drains
/// `r2`.
#[tokio::test]
async fn a_send_left_queued_behind_an_ended_run_drains_on_open() {
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let gate = Arc::new(Notify::new());
    let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let mut gated = executor(vec![text_step("a1", "hello")], Some(Arc::clone(&gate)));
    gated.drain_gate = Some((Arc::clone(&reached), release));
    let (commands, served, backend) = actor_with(gated, parts.clone()).await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        send(&commands, "r2", "second").await,
        SendOutcome::Pending { .. }
    ));
    gate.notify_one();
    reached.notified().await;
    served.abort();
    let _ = served.await;
    let executor = Arc::new(executor(vec![text_step("a2", "again")], None));
    let (commands, served) = reopen(backend, executor, &parts).await;
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let run = session.operations().run.as_ref().unwrap();
    assert_eq!(run.run_id, "r2");
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert!(session.operations().queue.is_empty());
    assert_eq!(texts(&session), ["first", "hello", "second", "again"]);
}

/// A fetch plan's manifest for these tests, with `plan` as the plan the caller sent: the tools
/// `read` and `edit`, and a system text.
fn plan_manifest(identity: &str, plan: serde_json::Value) -> broca_wal::PlanManifest {
    broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: identity.into(),
        composition_digest: "composition".into(),
        tools: Arc::new(
            ["read", "edit"]
                .into_iter()
                .map(|name| {
                    serde_json::from_value(serde_json::json!({
                        "name": name,
                        "description": "a tool",
                        "input_schema": {"type": "object"},
                    }))
                    .unwrap()
                })
                .collect(),
        ),
        system_text: "you are a worker".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    }
}

/// A send carrying a plan, as the serve layer hands it over: its identity, and the manifest it
/// fetched when the session had no run.
async fn send_planned(
    commands: &mpsc::UnboundedSender<LineageCmd>,
    run_id: &str,
    request: SubmissionRequest,
    identity: &str,
    manifest: Option<broca_wal::PlanManifest>,
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
            request: Box::new(request),
            kind: RunKind::FreshOrResume,
            key: SendKey::Minted,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: Some(Box::new(crate::actor::SendPlan {
                identity: identity.into(),
                manifest: manifest.map(Arc::new),
                model_check: None,
            })),
            order: None,
            reply,
        })
        .unwrap();
    answer.await.unwrap()
}

/// A send carrying no tools of its own, as a planned session's sends do.
fn untooled(text: &str) -> SubmissionRequest {
    let mut request = request(text);
    request.tools = None;
    request.policy_allowed = Default::default();
    request
}

/// The session's first send freezes the plan its send fetched: the run is sent the manifest's
/// tools and opens with its system text, its start records the manifest, and a later plain send
/// runs under the same manifest with no system text of its own.
#[tokio::test]
async fn a_planned_session_runs_on_the_manifests_tools_and_system_text() {
    let executor = Arc::new(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        None,
    ));
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = StoreActor::new(bind(), session, executor.clone(), Box::new(parts.clone()));
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    let manifest = plan_manifest("plan-1", serde_json::json!({}));
    assert!(matches!(
        send_planned(
            &commands,
            "r1",
            untooled("first"),
            "plan-1",
            Some(manifest.clone())
        )
        .await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert!(matches!(
        send(&commands, "r2", "second").await,
        SendOutcome::DeliveryRefused { field: "tools", .. }
    ));
    assert!(matches!(
        send_with(&commands, "r2", untooled("second"), SendKey::Minted).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let calls = executor.provider.inner.calls();
    assert_eq!(calls.len(), 2);
    for call in &calls {
        let names: Vec<_> = call.tools.iter().map(|tool| tool.name.as_str()).collect();
        assert_eq!(names, ["read", "edit"]);
    }
    assert_eq!(
        calls[0].prompt[0].role,
        broca_protocol::content::Role::System
    );
    assert_eq!(
        calls[0].prompt[0].content,
        [ContentBlock::Text {
            text: "you are a worker".into()
        }]
    );
    let systems = |call: &broca_protocol::request::CallOptions| {
        call.prompt
            .iter()
            .filter(|message| message.role == broca_protocol::content::Role::System)
            .count()
    };
    assert_eq!(systems(&calls[1]), 1, "the later run adds no system text");
    for start in [
        session.operations().owner.first_start.as_ref().unwrap(),
        session
            .operations()
            .run
            .as_ref()
            .unwrap()
            .start
            .as_ref()
            .unwrap(),
    ] {
        let admission = broca_session_loop::run::admission_of_start(start, &parts).unwrap();
        assert_eq!(
            admission.manifest.as_deref(),
            Some(&manifest),
            "{}",
            start.run_id
        );
    }
}

/// A later send's plan must be the one the session froze: the same plan runs as an ordinary
/// send, another is refused naming both, and nothing is written for the refusal.
#[tokio::test]
async fn a_later_send_with_another_plan_is_refused() {
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        None,
    ))
    .await;
    let manifest = plan_manifest("plan-1", serde_json::json!({}));
    assert!(matches!(
        send_planned(&commands, "r1", untooled("first"), "plan-1", Some(manifest)).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    assert!(matches!(
        send_planned(&commands, "r2", untooled("second"), "plan-2", None).await,
        SendOutcome::PlanRefused(broca_fetch_plan::Refusal::PlanDrift { .. })
    ));
    assert!(matches!(
        send_planned(&commands, "r3", untooled("third"), "plan-1", None).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(starts(&session).await, 2, "the refused send started no run");
    // The first run opens with the manifest's system text, as its input recorded it.
    assert_eq!(
        texts(&session),
        ["you are a worker", "first", "hello", "third", "again"]
    );
}

/// A plan that confines its tools can't start a session that has no scope: the refusal names
/// the preset, and nothing is written.
#[tokio::test]
async fn a_confining_plan_without_a_scope_is_refused() {
    let (commands, served, _) = actor(executor(vec![text_step("a1", "hello")], None)).await;
    let manifest = plan_manifest(
        "plan-1",
        serde_json::json!({"tool_items": [{"preset": "worker"}]}),
    );
    assert!(matches!(
        send_planned(&commands, "r1", untooled("first"), "plan-1", Some(manifest)).await,
        SendOutcome::ScopeRefused(broca_core::ScopeRefusal::ScopeRequired { preset: "worker" })
    ));
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(starts(&session).await, 0);
}

/// A send leaving its model to the session runs on the model the session's plan selected.
#[tokio::test]
async fn a_send_leaving_its_model_runs_on_the_plans_model() {
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        None,
    ))
    .await;
    let manifest = plan_manifest(
        "plan-1",
        serde_json::json!({"model": {"provider": "stub", "model": "planned"}}),
    );
    assert!(matches!(
        send_planned(&commands, "r1", untooled("first"), "plan-1", Some(manifest)).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    let mut inheriting = untooled("second");
    inheriting
        .inherit_frozen
        .insert(broca_protocol::request::InheritedParam::Model);
    assert!(matches!(
        send_with(&commands, "r2", inheriting, SendKey::Minted).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let start = session
        .operations()
        .run
        .as_ref()
        .unwrap()
        .start
        .as_ref()
        .unwrap();
    assert_eq!(start.run_id, "r2");
    assert_eq!(start.model.model_id, "planned");
}

/// A send queued behind a planned session's running run drains under the session's plan: its
/// run is sent the manifest's tools and records the manifest on its start.
#[tokio::test]
async fn a_queued_send_drains_under_the_sessions_plan() {
    let gate = Arc::new(Notify::new());
    let executor = Arc::new(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        Some(Arc::clone(&gate)),
    ));
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = StoreActor::new(bind(), session, executor.clone(), Box::new(parts.clone()));
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    let manifest = plan_manifest("plan-1", serde_json::json!({}));
    assert!(matches!(
        send_planned(
            &commands,
            "r1",
            untooled("first"),
            "plan-1",
            Some(manifest.clone())
        )
        .await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        send_with(&commands, "r2", untooled("second"), SendKey::Minted).await,
        SendOutcome::Pending { .. }
    ));
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let calls = executor.provider.inner.calls();
    assert_eq!(calls.len(), 2);
    let names: Vec<_> = calls[1]
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    assert_eq!(names, ["read", "edit"]);
    let start = session
        .operations()
        .run
        .as_ref()
        .unwrap()
        .start
        .as_ref()
        .unwrap();
    assert_eq!(start.run_id, "r2");
    let admission = broca_session_loop::run::admission_of_start(start, &parts).unwrap();
    assert_eq!(admission.manifest.as_deref(), Some(&manifest));
}

/// The deployment's setup parts store over `backend`, as a store session's actor uses it.
fn setup_storage(
    backend: &'static MemoryBackend,
) -> broca_session_loop::durable::SetupParts<&'static MemoryBackend> {
    broca_session_loop::durable::SetupParts {
        backend,
        key: deployment().setup_key(),
    }
}

/// The parts the deployment's setup store holds on `backend`, as a process loads them at start.
async fn loaded_parts(backend: &'static MemoryBackend) -> SharedParts {
    let key = deployment().setup_key();
    let loaded = broca_session_store::setup::SetupStore::new(&backend, &key)
        .load_all()
        .await
        .unwrap();
    SharedParts::new(Box::new(
        broca_session_loop::parts::DeploymentParts::loaded(loaded),
    ))
}

/// With durable setup storage, every part a run's start names is in the deployment's setup
/// store by the time the start is, and a process started afresh, holding only what the store
/// loads, reads the session's runs back: a planned session's next send runs on its frozen
/// manifest, and a queued send that drained under it too.
#[tokio::test]
async fn setup_parts_are_stored_before_the_records_naming_them() {
    let gate = Arc::new(Notify::new());
    let executor = Arc::new(executor(
        vec![
            text_step("a1", "hello"),
            text_step("a2", "again"),
            text_step("a3", "third"),
        ],
        Some(Arc::clone(&gate)),
    ));
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let parts = SharedParts::new(Box::new(
        broca_session_loop::parts::DeploymentParts::default(),
    ));
    let actor = StoreActor::new(bind(), session, executor.clone(), Box::new(parts))
        .with_storage(setup_storage(backend));
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    let manifest = plan_manifest("plan-1", serde_json::json!({}));
    assert!(matches!(
        send_planned(
            &commands,
            "r1",
            untooled("first"),
            "plan-1",
            Some(manifest.clone())
        )
        .await,
        SendOutcome::Active { .. }
    ));
    // The run's start is durable now, and nothing else has been appended: every part it names
    // must already be in the setup store.
    let stored_with_start: std::collections::BTreeSet<_> =
        broca_session_store::setup::SetupStore::new(&backend, &deployment().setup_key())
            .load_all()
            .await
            .unwrap()
            .into_iter()
            .map(|(digest, _)| digest.0)
            .collect();
    assert!(matches!(
        send_with(&commands, "r2", untooled("second"), SendKey::Minted).await,
        SendOutcome::Pending { .. }
    ));
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    drop(session);

    // A fresh process: only what the setup store holds.
    let parts = loaded_parts(backend).await;
    let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let first = session.operations().owner.first_start.as_ref().unwrap();
    assert!(!first.setup.is_empty());
    for part in &first.setup {
        assert!(
            stored_with_start.contains(&part.digest.0),
            "{part:?} was not stored before the run start naming it"
        );
    }
    for start in [
        first,
        session
            .operations()
            .run
            .as_ref()
            .unwrap()
            .start
            .as_ref()
            .unwrap(),
    ] {
        let admission = broca_session_loop::run::admission_of_start(start, &parts).unwrap();
        assert_eq!(
            admission.manifest.as_deref(),
            Some(&manifest),
            "{}",
            start.run_id
        );
    }
    let actor = StoreActor::new(bind(), session, executor.clone(), Box::new(parts))
        .with_storage(setup_storage(backend));
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    gate.notify_one();
    assert!(matches!(
        send_with(&commands, "r3", untooled("third"), SendKey::Minted).await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let start = session
        .operations()
        .run
        .as_ref()
        .unwrap()
        .start
        .as_ref()
        .unwrap();
    assert_eq!(start.run_id, "r3");
    let calls = executor.provider.inner.calls();
    let names: Vec<_> = calls[2]
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    assert_eq!(names, ["read", "edit"]);
}

/// A run still running when the actor stops, as at a module stop, ends `Interrupted` with
/// today's shutdown seal: no further model call, and never `Cancelled`, which records a
/// caller's decision nobody made. Nothing queued starts after it.
#[tokio::test]
async fn a_run_running_at_a_stop_ends_interrupted_by_the_shutdown_seal() {
    let gate = Arc::new(Notify::new());
    let executor = executor(
        vec![
            text_step("a1", "never finished"),
            text_step("a2", "never sent"),
        ],
        Some(Arc::clone(&gate)),
    );
    let (commands, served, _) = actor(executor).await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        send(&commands, "r2", "second").await,
        SendOutcome::Pending { .. }
    ));
    // The run waits on its first model call; the actor stops with it running. A stop that
    // waits on the run instead of ending it would never return, so it fails here by name.
    drop(commands);
    let session = tokio::time::timeout(std::time::Duration::from_secs(10), served)
        .await
        .expect("the actor stops without waiting on its running run")
        .unwrap();
    let run = session.operations().run.as_ref().unwrap();
    assert_eq!(run.run_id, "r1", "the queued send never started");
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Interrupted)
    );
    let ends: Vec<_> = session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::RunEnd(end) => Some(end),
            _ => None,
        })
        .collect();
    assert_eq!(ends.len(), 1, "{ends:?}");
    assert_eq!(ends[0].interrupt_reason.as_deref(), Some("runner_shutdown"));
    assert_eq!(session.operations().queue.len(), 1, "r2 stays queued");
}
