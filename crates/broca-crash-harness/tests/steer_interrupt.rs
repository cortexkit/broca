//! `session.send` with `delivery: steer` and `delivery: interrupt`, cut at every WAL record
//! boundary.
//!
//! Steers and interrupts are written by the session's lineage actor, not by the run loop alone,
//! so each scenario's reference run goes through the lineage registry the serve layer calls,
//! over the real core loop and a real file WAL. A scripted provider and tool plane hold the
//! run inside a model step, a tool call or a tool batch's close, and the test sends the steer or
//! interrupt at exactly that point.
//!
//! Every cut is then recovered the way a restarted daemon recovers it: the startup crash-orphan
//! pass marks the run the index would still call active, a fresh registry opens the session
//! (sealing that run `Interrupted`, or resuming it, and draining queued messages), and the test
//! waits for the session to settle. The invariants checked on what the recovery leaves:
//! - the durable prefix is untouched (nothing earlier is re-rendered or edited);
//! - every durable steer is in the transcript exactly once, placed after the tool result it
//!   arrived during, never between a tool call and its result, and read back as written;
//! - every episode has exactly one terminal, and an interrupted run ends `Cancelled` (or
//!   `Interrupted` when the process died before the cancel), and only after the interrupting
//!   message was durable;
//! - every durable queued message runs exactly once, interrupts ahead of plain sends;
//! - no tool call with a durable dispatch intent is sent again;
//! - a steer never starts a turn of its own.

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use broca_core::{
    run_loop_on_writer_admitted, AllowAll, BatchCtx, DispatchResult, FileLeaseStore, FileWalStore,
    OutputEvent, Provider, RunError, RunRequest, RunSummary, StartMode, ToolManifest, ToolPlane,
    ToolPlaneError,
};
use broca_crash_harness::{check_tool_pairing, LogImage};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::serve::{SendRequest, SendResponse, ServeHandler, StatusResponse};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ExecutionMode, GenerationConfig, ModelHandle, StopCondition, Tool},
    BindIdentity, Delivery, FinishReason, MessageId, RunId, StepId, ToolCallId, ToolChoice, Usage,
};
use broca_wal::WalRecord;
use tokio::sync::{mpsc, Notify};

#[path = "it/boundary_cancel.rs"]
mod boundary_cancel;

// ---------------------------------------------------------------------------------------------
// The scripted provider and tool plane.
// ---------------------------------------------------------------------------------------------

/// Where the script can hold a run until the test releases it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Hold {
    /// Inside a model step, before it answers.
    Model,
    /// Inside a tool call, before it returns (its dispatch intent is durable).
    Tool,
    /// After a tool batch's turn barrier (`TurnFinished` is durable), before the next step.
    BatchEnd,
}

#[derive(Clone)]
enum Step {
    Text(&'static str),
    Call(&'static str),
}

#[derive(Default)]
struct Script {
    steps: Mutex<VecDeque<Step>>,
    holds: Mutex<HashSet<Hold>>,
    gate: Notify,
    entered: Notify,
    dispatched: Mutex<Vec<String>>,
    pause_next_run: Mutex<bool>,
}

impl Script {
    fn with(steps: &[Step]) -> Arc<Self> {
        let script = Arc::new(Self::default());
        script.steps.lock().unwrap().extend(steps.iter().cloned());
        script
    }

    fn hold(&self, at: Hold, on: bool) {
        let mut holds = self.holds.lock().unwrap();
        if on {
            holds.insert(at);
        } else {
            holds.remove(&at);
            self.gate.notify_waiters();
        }
    }

    async fn wait_if_held(&self, at: Hold) {
        loop {
            let gate = self.gate.notified();
            if !self.holds.lock().unwrap().contains(&at) {
                return;
            }
            self.entered.notify_one();
            gate.await;
        }
    }
}

struct ScriptedProvider(Arc<Script>);

#[async_trait]
impl Provider for ScriptedProvider {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.0.wait_if_held(Hold::Model).await;
        let step = self
            .0
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Step::Text("done"));
        let (content, finish_reason) = match step {
            Step::Text(text) => (
                vec![ContentBlock::Text { text: text.into() }],
                FinishReason::Stop,
            ),
            Step::Call(id) => (
                vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId(id.into()),
                    tool_name: "work".into(),
                    input: serde_json::json!({}),
                    thought_signature: None,
                })],
                FinishReason::ToolCalls,
            ),
        };
        let (tx, rx) = mpsc::channel(4);
        tx.send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m".into()),
                    content,
                },
            },
        ))
        .await
        .unwrap();
        tx.send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason,
            retries_used: 0,
        }))
        .await
        .unwrap();
        rx
    }
}

struct ScriptedTools(Arc<Script>);

#[async_trait]
impl ToolPlane for ScriptedTools {
    async fn batch_started(
        &self,
        _ctx: &BatchCtx,
        _calls: &[ToolCall],
        _manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }

    async fn dispatch(&self, _ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.0
            .dispatched
            .lock()
            .unwrap()
            .push(call.tool_call_id.0.clone());
        self.0.wait_if_held(Hold::Tool).await;
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "worked".into(),
            },
            is_error: false,
        })
    }

    async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        self.0.wait_if_held(Hold::BatchEnd).await;
        Ok(())
    }
}

struct CoreExec {
    wal_store: Arc<FileWalStore>,
    script: Arc<Script>,
}

#[async_trait]
impl RunExecutor for CoreExec {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        let mut control = job.control;
        if std::mem::take(&mut *self.script.pause_next_run.lock().unwrap()) {
            control.pause_for_auth_at_start = Some(broca_protocol::ProviderError {
                provider_details: None,
                provider_subcode: None,
                refusal: None,
                class: broca_protocol::ErrorClass::Auth,
                message: "credential needs re-authentication".into(),
                status: None,
                retry_after_secs: None,
                provider_code: Some("needs_reauth".into()),
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            });
        }
        run_loop_on_writer_admitted(
            job.req,
            &job.writer,
            mode,
            &ScriptedProvider(self.script.clone()),
            &ScriptedTools(self.script.clone()),
            &AllowAll,
            self.wal_store.as_ref(),
            None,
            control,
            &out,
            job.on_admitted,
        )
        .await
    }
}

// ---------------------------------------------------------------------------------------------
// A session on one state root.
// ---------------------------------------------------------------------------------------------

fn session() -> BindIdentity {
    BindIdentity::new("/broca-steer-harness", "runner", "s")
}

struct Session {
    root: PathBuf,
    wal: Arc<FileWalStore>,
    registry: LineageRegistry,
    script: Arc<Script>,
}

impl Session {
    fn open(root: PathBuf, script: Arc<Script>) -> Self {
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal.clone(),
            Arc::new(CoreExec {
                wal_store: wal.clone(),
                script: script.clone(),
            }),
        );
        let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal.clone(), None);
        Self {
            root,
            wal,
            registry,
            script,
        }
    }

    fn wal_path(&self) -> PathBuf {
        self.wal.wal_path(&session())
    }

    fn log(&self) -> LogImage {
        LogImage::read(&self.wal_path())
    }

    async fn send(&self, send_id: &str, text: &str, delivery: Delivery) -> SendResponse {
        let request = RunRequest {
            input_written_at_ms: None,
            directives: None,
            session_class: None,
            retention: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            scope: None,
            project_id: None,
            principal: None,
            send_id: None,
            request_identity_digest: None,
            run_id: RunId(format!("run-{send_id}")),
            session: session(),
            model: ModelHandle::new("stub", "m"),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text { text: text.into() }],
            )],
            tools: vec![Tool {
                name: "work".into(),
                description: "does work".into(),
                input_schema: serde_json::json!({"type": "object"}),
                module: None,
                execution_mode: ExecutionMode::Parallel,
                reply: None,
            }],
            policy_allowed: ["work".to_string()].into_iter().collect(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: vec![StopCondition::StepCount { n: 10 }],
            submission_id: None,
            budget: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen_cache: false,
            structured_output: None,
        };
        self.registry
            .send(SendRequest {
                session_class: None,
                retention: None,
                stop_sequences_named: false,
                title: None,
                conversation_key: None,
                session: session(),
                run_id: request.run_id.clone(),
                send_id: Some(send_id.into()),
                request: Box::new(request.to_submission()),
                skip_materialization: false,
                kind: RunKind::FreshOrResume,
                delivery,
                plan: None,
                order: None,
            })
            .await
            .unwrap_or_else(|error| panic!("send {send_id}: {error}"))
    }

    async fn wait_entered(&self) {
        broca_test_support::wait_for(
            "run reached the held point",
            self.script.entered.notified(),
            || format!("WAL records={}", self.log().records().len()),
        )
        .await;
    }

    /// Wait until the actor is idle, nothing is queued, and every episode has ended (or is
    /// paused): nothing more will be written without a new send.
    async fn settle(&self) -> LogImage {
        broca_test_support::wait_until("idle session with no pending or open episodes", || async {
            let idle = matches!(
                self.registry.status(&session()).await,
                Ok(StatusResponse::Idle)
            );
            let log = self.log();
            let records = log.records();
            if idle && broca_core::pending_submissions(&records).is_empty() && all_ended(&records) {
                return Ok(log);
            }
            Err(format!(
                "root={}, idle={idle}, records={}, pending={}, all_ended={}",
                self.root.display(),
                records.len(),
                broca_core::pending_submissions(&records).len(),
                all_ended(&records)
            ))
        })
        .await
    }
}

fn all_ended(records: &[WalRecord]) -> bool {
    let mut open = false;
    for record in records {
        match record {
            WalRecord::RunStarted { .. } => open = true,
            WalRecord::RunFinished { .. } | WalRecord::RunPaused { .. } => open = false,
            WalRecord::StepStarted { .. } => {}
            _ => {}
        }
    }
    !open
}

// ---------------------------------------------------------------------------------------------
// Scenarios: each drives a reference run and says which messages it sent.
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Scenario {
    /// A steer sent while a tool call runs.
    SteerDuringToolCall,
    /// A steer sent between two model steps (after the first turn's barrier).
    SteerBetweenSteps,
    /// An interrupt sent while a model step streams.
    InterruptDuringModelStream,
    /// An interrupt sent while a tool call runs.
    InterruptDuringToolCall,
    /// Plain P1 and P2 queued behind a running turn, then an interrupt I.
    InterruptAheadOfQueued,
    /// An interrupt into a session whose run is paused for re-authentication.
    InterruptPausedRun,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Scenario::SteerDuringToolCall => "steer-during-tool-call",
            Scenario::SteerBetweenSteps => "steer-between-steps",
            Scenario::InterruptDuringModelStream => "interrupt-during-model-stream",
            Scenario::InterruptDuringToolCall => "interrupt-during-tool-call",
            Scenario::InterruptAheadOfQueued => "interrupt-ahead-of-queued",
            Scenario::InterruptPausedRun => "interrupt-paused-run",
        }
    }

    fn steps(self) -> Vec<Step> {
        match self {
            Scenario::SteerDuringToolCall
            | Scenario::SteerBetweenSteps
            | Scenario::InterruptDuringToolCall => vec![Step::Call("call-1"), Step::Text("answer")],
            _ => vec![],
        }
    }

    async fn reference(self, session: &Session) {
        let script = &session.script;
        match self {
            Scenario::SteerDuringToolCall => {
                script.hold(Hold::Tool, true);
                session.send("turn-1", "task", Delivery::Queue).await;
                session.wait_entered().await;
                session.send("steer-1", "steered", Delivery::Steer).await;
                script.hold(Hold::Tool, false);
            }
            Scenario::SteerBetweenSteps => {
                script.hold(Hold::BatchEnd, true);
                session.send("turn-1", "task", Delivery::Queue).await;
                session.wait_entered().await;
                session.send("steer-1", "steered", Delivery::Steer).await;
                script.hold(Hold::BatchEnd, false);
            }
            Scenario::InterruptDuringModelStream => {
                script.hold(Hold::Model, true);
                session.send("turn-1", "task", Delivery::Queue).await;
                session.wait_entered().await;
                session
                    .send("stop-1", "interrupting", Delivery::Interrupt)
                    .await;
                script.hold(Hold::Model, false);
            }
            Scenario::InterruptDuringToolCall => {
                script.hold(Hold::Tool, true);
                session.send("turn-1", "task", Delivery::Queue).await;
                session.wait_entered().await;
                session
                    .send("stop-1", "interrupting", Delivery::Interrupt)
                    .await;
                script.hold(Hold::Tool, false);
            }
            Scenario::InterruptAheadOfQueued => {
                script.hold(Hold::Model, true);
                session.send("turn-0", "task", Delivery::Queue).await;
                session.wait_entered().await;
                session.send("p1", "plain one", Delivery::Queue).await;
                session.send("p2", "plain two", Delivery::Queue).await;
                session.send("i", "interrupting", Delivery::Interrupt).await;
                script.hold(Hold::Model, false);
            }
            Scenario::InterruptPausedRun => {
                *script.pause_next_run.lock().unwrap() = true;
                session.send("turn-1", "task", Delivery::Queue).await;
                session.settle().await;
                session
                    .send("stop-1", "interrupting", Delivery::Interrupt)
                    .await;
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The invariants.
// ---------------------------------------------------------------------------------------------

fn text_of(message: &Message) -> Option<&str> {
    match message.content.first() {
        Some(ContentBlock::Text { text }) => Some(text),
        _ => None,
    }
}

fn steered_texts(records: &[WalRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::Steered { input, .. } => input.first().and_then(text_of).map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// Each episode as (run id, submission id, terminal reasons) in WAL order.
fn episodes_of(records: &[WalRecord]) -> Vec<(String, Option<String>, Vec<String>)> {
    let mut out: Vec<(String, Option<String>, Vec<String>)> = Vec::new();
    for record in records {
        match record {
            WalRecord::RunStarted {
                run_id,
                submission_id,
                ..
            } => out.push((
                run_id.0.clone(),
                submission_id.as_ref().map(|id| id.0.clone()),
                Vec::new(),
            )),
            WalRecord::RunFinished { reason, .. } => {
                if let Some(last) = out.last_mut() {
                    last.2.push(
                        serde_json::to_value(reason)
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_owned(),
                    );
                }
            }
            _ => {}
        }
    }
    out
}

fn queued(records: &[WalRecord]) -> Vec<(String, Option<Delivery>)> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::Queued {
                submission_id,
                delivery,
                ..
            } => Some((submission_id.0.clone(), *delivery)),
            _ => None,
        })
        .collect()
}

fn intents(records: &[WalRecord]) -> HashSet<String> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ToolDispatchIntent { tool_call_id, .. } => Some(tool_call_id.0.clone()),
            _ => None,
        })
        .collect()
}

/// Check what recovering `cut` left (`after`), with the recovery's own tool dispatches.
fn check(
    label: &str,
    scenario: Scenario,
    cut: &LogImage,
    after: &LogImage,
    redispatched: &[String],
) {
    let before = cut.records();
    let records = after.records();

    // The durable prefix is untouched: recovery only appends.
    assert!(
        after.bytes.starts_with(&cut.bytes),
        "{label}: recovery rewrote the durable prefix"
    );

    // Steers: read back as written, never re-run through hooks, never added by recovery.
    let steers_before: Vec<&WalRecord> = before
        .iter()
        .filter(|record| matches!(record, WalRecord::Steered { .. }))
        .collect();
    let steers_after: Vec<&WalRecord> = records
        .iter()
        .filter(|record| matches!(record, WalRecord::Steered { .. }))
        .collect();
    assert_eq!(
        steers_after, steers_before,
        "{label}: recovery wrote a steer"
    );
    for steer in &steers_after {
        let WalRecord::Steered { hooks, .. } = steer else {
            unreachable!()
        };
        assert!(hooks.is_none(), "{label}: a steer gained hook output");
    }

    // The transcript: provider-valid, every durable steer exactly once, after the tool result
    // it arrived during.
    let replayed = broca_core::replay(&records)
        .unwrap_or_else(|error| panic!("{label}: replay failed: {error}"));
    let prompt = &replayed.prompt;
    check_tool_pairing(prompt).unwrap_or_else(|error| {
        panic!("{label}: a steer or message sits between a call and its result: {error}")
    });
    for text in steered_texts(&before) {
        let at: Vec<usize> = prompt
            .iter()
            .enumerate()
            .filter(|(_, message)| message.role == Role::User && text_of(message) == Some(&text))
            .map(|(index, _)| index)
            .collect();
        assert_eq!(
            at.len(),
            1,
            "{label}: steer {text:?} delivered {} times",
            at.len()
        );
        if matches!(scenario, Scenario::SteerDuringToolCall) {
            let result_at = prompt.iter().position(|message| message.role == Role::Tool);
            if let Some(result_at) = result_at {
                assert!(
                    at[0] > result_at,
                    "{label}: the steer precedes the result of the call it arrived during"
                );
            }
        }
    }

    // Every episode has exactly one terminal (a paused one none, and only while paused).
    let episodes = episodes_of(&records);
    let last = episodes.len().saturating_sub(1);
    for (index, (run_id, _, endings)) in episodes.iter().enumerate() {
        let paused_tail =
            index == last && matches!(records.last(), Some(WalRecord::RunPaused { .. }));
        if paused_tail {
            assert!(
                endings.is_empty(),
                "{label}: run {run_id} paused after a terminal"
            );
        } else {
            assert_eq!(
                endings.len(),
                1,
                "{label}: run {run_id} has terminals {endings:?}"
            );
        }
    }

    // A run is cancelled only by an interrupt whose message was already durable.
    for (index, record) in records.iter().enumerate() {
        if let WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Cancelled,
            ..
        } = record
        {
            assert!(
                records[..index].iter().any(|earlier| matches!(
                    earlier,
                    WalRecord::Queued {
                        delivery: Some(Delivery::Interrupt),
                        ..
                    }
                )),
                "{label}: a run was cancelled before the interrupting message was durable"
            );
        }
    }

    // Every durable queued message runs exactly once, interrupts first.
    let durable_queue = queued(&before);
    for (submission, _) in &durable_queue {
        let runs = episodes
            .iter()
            .filter(|(_, consumed, _)| consumed.as_deref() == Some(submission))
            .count();
        assert_eq!(runs, 1, "{label}: {submission} ran {runs} times");
    }
    if let Some((interrupt, _)) = durable_queue
        .iter()
        .find(|(_, delivery)| *delivery == Some(Delivery::Interrupt))
    {
        // The run the interrupt found active ends Cancelled, or Interrupted when the process
        // died before the cancel; then the interrupt runs before any plain queued message.
        let interrupted = episodes
            .iter()
            .position(|(_, consumed, _)| consumed.as_deref() == Some(interrupt))
            .expect("the interrupt ran")
            - 1;
        let reason = &episodes[interrupted].2[0];
        assert!(
            reason == "cancelled" || reason == "interrupted",
            "{label}: the interrupted run ended {reason}"
        );
        // Among the messages the recovery drained (not those already started in the cut), the
        // interrupt comes first.
        let started_in_cut: HashSet<String> = episodes_of(&before)
            .into_iter()
            .filter_map(|(_, consumed, _)| consumed)
            .collect();
        let drained: Vec<&str> = episodes
            .iter()
            .filter_map(|(_, consumed, _)| consumed.as_deref())
            .filter(|consumed| !started_in_cut.contains(*consumed))
            .collect();
        if drained.contains(&interrupt.as_str()) {
            assert_eq!(
                drained.first(),
                Some(&interrupt.as_str()),
                "{label}: a plain send ran ahead of the interrupt: {drained:?}"
            );
        }
    }

    // No turn of its own for a steer: new episodes are exactly the drained queued messages.
    let consumed_in_cut = episodes_of(&before)
        .iter()
        .filter(|(_, consumed, _)| consumed.is_some())
        .count();
    let retracted_in_cut = before
        .iter()
        .filter(|record| matches!(record, WalRecord::Retracted { .. }))
        .count();
    assert_eq!(
        episodes.len(),
        episodes_of(&before).len() + durable_queue.len() - consumed_in_cut - retracted_in_cut,
        "{label}: recovery started a turn nobody queued"
    );

    // Zero tool re-dispatch: nothing with a durable intent is sent again.
    let intended = intents(&before);
    for id in redispatched {
        assert!(
            !intended.contains(id),
            "{label}: tool call {id} was sent again"
        );
    }

    // A crash right after a durable steer, before the next step: the sweep seals the run
    // Interrupted, and the steer is placed once (checked above) with no new turn.
    if matches!(before.last(), Some(WalRecord::Steered { .. })) {
        assert_eq!(
            episodes.last().map(|episode| episode.2.clone()),
            Some(vec!["interrupted".to_owned()]),
            "{label}: the run a steer was pending in must be sealed Interrupted"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// The driver.
// ---------------------------------------------------------------------------------------------

fn scratch(tag: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "broca-steer-harness-{}-{tag}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

/// The last episode's run id when that episode has neither ended nor paused: the run the run
/// index would still call active after a crash.
fn active_run(records: &[WalRecord]) -> Option<RunId> {
    let mut active = None;
    for record in records {
        match record {
            WalRecord::RunStarted { run_id, .. } => active = Some(run_id.clone()),
            WalRecord::RunFinished { .. } | WalRecord::RunPaused { .. } => active = None,
            _ => {}
        }
    }
    active
}

async fn recover(scenario: Scenario, cut: &[u8]) -> (LogImage, LogImage, Vec<String>) {
    let root = scratch(scenario.name());
    let recovery = Session::open(root, Script::with(&[]));
    let path = recovery.wal_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, cut).unwrap();
    let cut = recovery.log();
    // The startup pass, as a restarted daemon runs it: the run the index still calls active
    // is marked, and opening the session seals it inside the actor before any command.
    let candidates: Vec<(RunId, BindIdentity)> = active_run(&cut.records())
        .map(|run_id| (run_id, session()))
        .into_iter()
        .collect();
    let started = SystemTime::now() + Duration::from_secs(1);
    let sweep = recovery
        .registry
        .mark_crash_orphans(recovery.wal.as_ref(), candidates, started);
    recovery.registry.seal_marked_crash_orphans(sweep).await;
    let after = recovery.settle().await;
    let redispatched = recovery.script.dispatched.lock().unwrap().clone();
    let _ = std::fs::remove_dir_all(&recovery.root);
    (cut, after, redispatched)
}

async fn every_record_boundary(scenario: Scenario) -> usize {
    let reference = Session::open(scratch(scenario.name()), Script::with(&scenario.steps()));
    scenario.reference(&reference).await;
    let log = reference.settle().await;
    let total = log.record_count();
    // The cuts that make each scenario worth running, counted so a change that stops reaching
    // them fails here rather than passing vacuously.
    let mut steer_pending_at_crash = 0;
    let mut interrupt_durable_run_unfinished = 0;
    let mut cancelled_before_new_run = 0;
    for records in 0..=total {
        let cut = log.bytes[..log.end_of_records(records)].to_vec();
        let (cut, after, redispatched) = recover(scenario, &cut).await;
        let before = cut.records();
        if matches!(before.last(), Some(WalRecord::Steered { .. })) {
            steer_pending_at_crash += 1;
        }
        let interrupt_durable = queued(&before)
            .iter()
            .any(|(_, delivery)| *delivery == Some(Delivery::Interrupt));
        let last_ended = matches!(
            before.iter().rev().find(|record| matches!(
                record,
                WalRecord::RunStarted { .. }
                    | WalRecord::RunFinished { .. }
                    | WalRecord::RunPaused { .. }
            )),
            Some(WalRecord::RunFinished { .. })
        );
        if interrupt_durable && !last_ended && episodes_of(&before).len() == 1 {
            interrupt_durable_run_unfinished += 1;
        }
        if interrupt_durable && last_ended && episodes_of(&before).len() == 1 {
            cancelled_before_new_run += 1;
        }
        check(
            &format!("{} cut after {records} of {total} records", scenario.name()),
            scenario,
            &cut,
            &after,
            &redispatched,
        );
    }
    let _ = std::fs::remove_dir_all(&reference.root);
    match scenario {
        Scenario::SteerDuringToolCall | Scenario::SteerBetweenSteps => assert!(
            steer_pending_at_crash > 0,
            "{}: no cut fell right after the steer",
            scenario.name()
        ),
        Scenario::InterruptAheadOfQueued => assert!(interrupt_durable_run_unfinished > 0),
        _ => {
            assert!(
                interrupt_durable_run_unfinished > 0,
                "{}: no cut between the interrupt's Queued and the cancel",
                scenario.name()
            );
            assert!(
                cancelled_before_new_run > 0,
                "{}: no cut between the cancel and the new run",
                scenario.name()
            );
        }
    }
    total
}

fn assert_reference(scenario: Scenario, records: &[WalRecord]) {
    let endings: Vec<Vec<String>> = episodes_of(records).into_iter().map(|e| e.2).collect();
    match scenario {
        Scenario::SteerDuringToolCall | Scenario::SteerBetweenSteps => {
            assert_eq!(endings, [vec!["completed".to_owned()]]);
            assert_eq!(steered_texts(records), ["steered"]);
        }
        Scenario::InterruptDuringModelStream | Scenario::InterruptDuringToolCall => {
            assert_eq!(
                endings,
                [vec!["cancelled".to_owned()], vec!["completed".to_owned()]]
            );
        }
        Scenario::InterruptAheadOfQueued => {
            let order: Vec<Option<String>> =
                episodes_of(records).into_iter().map(|e| e.1).collect();
            assert_eq!(
                order,
                [
                    None,
                    Some("sub-run-i".to_owned()),
                    Some("sub-run-p1".to_owned()),
                    Some("sub-run-p2".to_owned())
                ]
            );
        }
        Scenario::InterruptPausedRun => {
            assert_eq!(
                endings,
                [vec!["cancelled".to_owned()], vec!["completed".to_owned()]]
            );
        }
    }
}

async fn run_scenario(scenario: Scenario) {
    // The reference itself must show the scenario's intended shape before its cuts mean much.
    let reference = Session::open(scratch(scenario.name()), Script::with(&scenario.steps()));
    scenario.reference(&reference).await;
    let log = reference.settle().await;
    assert_reference(scenario, &log.records());
    let _ = std::fs::remove_dir_all(&reference.root);
    let total = every_record_boundary(scenario).await;
    assert!(total > 5, "{}: too few records to cut", scenario.name());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn steer_during_a_tool_call_at_every_record_boundary() {
    run_scenario(Scenario::SteerDuringToolCall).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn steer_between_two_model_steps_at_every_record_boundary() {
    run_scenario(Scenario::SteerBetweenSteps).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupt_during_a_model_stream_at_every_record_boundary() {
    run_scenario(Scenario::InterruptDuringModelStream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupt_during_a_tool_call_at_every_record_boundary() {
    run_scenario(Scenario::InterruptDuringToolCall).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupt_ahead_of_queued_sends_at_every_record_boundary() {
    run_scenario(Scenario::InterruptAheadOfQueued).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupt_of_a_paused_run_at_every_record_boundary() {
    run_scenario(Scenario::InterruptPausedRun).await;
}

// ---------------------------------------------------------------------------------------------
// A real process kill while a steer's message lives in the running turn's inbox.
// ---------------------------------------------------------------------------------------------

/// The child process's state root, set only in a child spawned by the kill below.
const ENV_STEER_KILL_ROOT: &str = "BROCA_STEER_KILL_ROOT";
const STEER_KILL_CHILD: &str = "steer_kill_child";

fn steer_kill_marker(root: &std::path::Path) -> PathBuf {
    root.join("steer-durable")
}

/// The runner the SIGKILL test kills: the whole lineage actor and core loop in their own
/// process. It holds a tool call open, steers a message in (durable before the reply, and
/// otherwise only in the run's in-memory inbox), announces that, and waits to be killed before
/// the run reaches its next step. A no-op in an ordinary test run.
#[test]
fn steer_kill_child() {
    let Ok(root) = std::env::var(ENV_STEER_KILL_ROOT) else {
        return;
    };
    let root = PathBuf::from(root);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runner runtime");
    runtime.block_on(async {
        let session = Session::open(
            root.clone(),
            Script::with(&[Step::Call("call-1"), Step::Text("answer")]),
        );
        session.script.hold(Hold::Tool, true);
        session.send("turn-1", "task", Delivery::Queue).await;
        session.wait_entered().await;
        session.send("steer-1", "steered", Delivery::Steer).await;
        std::fs::write(steer_kill_marker(&root), b"").expect("announce the durable steer");
        std::future::pending::<()>().await;
    });
}

/// A steer made durable while a tool call is held, then the process holding the run's inbox is
/// killed with SIGKILL before the next `StepStarted`. A daemon restarted on the same state root
/// recovers it: the steer is in the transcript exactly once, after the tool call's result, and
/// the tool call is never dispatched again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_steer_held_in_memory_survives_a_sigkill_before_the_next_step() {
    let root = scratch("steer-sigkill");
    let marker = steer_kill_marker(&root);
    let method = broca_crash_harness::kill_child_test_at_marker(
        "steer-sigkill",
        STEER_KILL_CHILD,
        &[(ENV_STEER_KILL_ROOT, root.as_os_str())],
        &marker,
    );
    let wal_path = FileWalStore::new(root.join("wal")).wal_path(&session());
    let durable = LogImage::read(&wal_path);
    let before = durable.records();

    // What the kill left: the call's intent and the steer are durable, and nothing after the
    // steer, so its message had reached the model only through the dead process's memory.
    let steer_at = before
        .iter()
        .position(|record| matches!(record, WalRecord::Steered { .. }))
        .expect("the steer is durable");
    assert!(before
        .iter()
        .any(|record| matches!(record, WalRecord::ToolDispatchIntent { .. })));
    assert!(!before
        .iter()
        .any(|record| matches!(record, WalRecord::ToolResult { .. })));
    assert!(!before[steer_at..]
        .iter()
        .any(|record| matches!(record, WalRecord::StepStarted { .. })));

    let mut suite = broca_crash_harness::Suite::new("steer-sigkill");
    suite.record([broca_crash_harness::KillReport {
        scenario: "steer-sigkill",
        point: broca_crash_harness::KillPoint::InFlight(
            broca_crash_harness::InFlight::ToolDispatch(ToolCallId("call-1".into())),
        ),
        method,
        after_sync: None,
        inside_resume_from: None,
        state_root: root.clone(),
        durable: durable.clone(),
    }]);
    assert_eq!(suite.finish().get("Sigkill"), Some(&1));

    // The restart, on the same state root, recovering as the daemon does.
    let recovery = Session::open(root.clone(), Script::with(&[]));
    let candidates: Vec<(RunId, BindIdentity)> = active_run(&before)
        .map(|run_id| (run_id, session()))
        .into_iter()
        .collect();
    let started = SystemTime::now() + Duration::from_secs(1);
    let sweep = recovery
        .registry
        .mark_crash_orphans(recovery.wal.as_ref(), candidates, started);
    recovery.registry.seal_marked_crash_orphans(sweep).await;
    let after = recovery.settle().await;
    let redispatched = recovery.script.dispatched.lock().unwrap().clone();

    // Every harness invariant, then the three this kill is about, stated directly.
    check(
        "steer-sigkill",
        Scenario::SteerDuringToolCall,
        &durable,
        &after,
        &redispatched,
    );
    let prompt = broca_core::replay(&after.records()).unwrap().prompt;
    let steered: Vec<usize> = prompt
        .iter()
        .enumerate()
        .filter(|(_, message)| message.role == Role::User && text_of(message) == Some("steered"))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(
        steered.len(),
        1,
        "the steer is in the transcript exactly once"
    );
    let result_at = prompt
        .iter()
        .position(|message| message.role == Role::Tool)
        .expect("the held call is paired with a result");
    assert!(steered[0] > result_at, "the steer follows the tool result");
    assert!(
        redispatched.is_empty(),
        "the tool call was dispatched again: {redispatched:?}"
    );

    // The steer's delivery receipt, answered by the restarted daemon from the WAL alone. The
    // killed run was sealed before any step rendered the steer, so a retry carries no receipt
    // (never `pending` or `unknown`). The next episode's prefix carries it, so once that episode
    // has started a step the retry answers `turn`, naming the new run, and keeps answering it.
    let retry = recovery.send("steer-1", "steered", Delivery::Steer).await;
    assert!(
        matches!(
            &retry,
            SendResponse::Finished { run_id, delivered: None, .. } if run_id == "run-turn-1"
        ),
        "{retry:?}"
    );
    recovery.send("turn-2", "next", Delivery::Queue).await;
    recovery.settle().await;
    let expected = cortexkit_role_llm_runner::send::Delivered::new("turn").with_ref("run-turn-2");
    for _ in 0..2 {
        match recovery.send("steer-1", "steered", Delivery::Steer).await {
            SendResponse::Finished {
                run_id, delivered, ..
            } => {
                assert_eq!(run_id, "run-turn-1");
                assert_eq!(delivered.as_ref(), Some(&expected));
            }
            other => panic!("{other:?}"),
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------------------------
// A steer's delivery receipt across a real process kill.
// ---------------------------------------------------------------------------------------------

const ENV_RECEIPT_KILL_ROOT: &str = "BROCA_STEER_RECEIPT_KILL_ROOT";
const RECEIPT_KILL_CHILD: &str = "steer_receipt_kill_child";

/// Where the child writes the receipt it answered, then the marker the kill waits for.
fn receipt_answer(root: &std::path::Path) -> PathBuf {
    root.join("receipt.json")
}

fn receipt_kill_marker(root: &std::path::Path) -> PathBuf {
    root.join("receipt-answered")
}

/// The runner the receipt test kills. It steers a message into a run held inside a tool call,
/// lets the run finish (its next step renders the steer), re-sends the steer, writes the answer
/// down, and waits to be killed. A no-op in an ordinary test run.
#[test]
fn steer_receipt_kill_child() {
    let Ok(root) = std::env::var(ENV_RECEIPT_KILL_ROOT) else {
        return;
    };
    let root = PathBuf::from(root);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runner runtime");
    runtime.block_on(async {
        let session = Session::open(
            root.clone(),
            Script::with(&[Step::Call("call-1"), Step::Text("answer")]),
        );
        session.script.hold(Hold::Tool, true);
        session.send("turn-1", "task", Delivery::Queue).await;
        session.wait_entered().await;
        session.send("steer-1", "steered", Delivery::Steer).await;
        session.script.hold(Hold::Tool, false);
        session.settle().await;
        let answer = session.send("steer-1", "steered", Delivery::Steer).await;
        std::fs::write(
            receipt_answer(&root),
            serde_json::to_vec(&answer).expect("encode the answer"),
        )
        .expect("write the answer");
        // Created only after the answer is fully written, so the parent never reads half of it.
        std::fs::write(receipt_kill_marker(&root), b"").expect("announce the answer");
        std::future::pending::<()>().await;
    });
}

/// A receipt once given is the receipt every later retry gets, after a SIGKILL and a restart
/// too: the restarted daemon derives it from the same durable WAL records the killed one did.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_steer_delivery_receipt_is_the_same_after_a_sigkill_and_restart() {
    let root = scratch("steer-receipt-sigkill");
    let method = broca_crash_harness::kill_child_test_at_marker(
        "steer-receipt-sigkill",
        RECEIPT_KILL_CHILD,
        &[(ENV_RECEIPT_KILL_ROOT, root.as_os_str())],
        &receipt_kill_marker(&root),
    );
    assert!(matches!(method, broca_crash_harness::KillMethod::Sigkill));
    let before: SendResponse =
        serde_json::from_slice(&std::fs::read(receipt_answer(&root)).unwrap()).unwrap();
    // The killed process answered `step` naming the run the steer joined: the run's next step
    // rendered it.
    let step = cortexkit_role_llm_runner::send::Delivered::new("step").with_ref("run-turn-1");
    match &before {
        SendResponse::Finished {
            run_id, delivered, ..
        } => {
            assert_eq!(run_id, "run-turn-1");
            assert_eq!(delivered.as_ref(), Some(&step));
        }
        other => panic!("{other:?}"),
    }

    let recovery = Session::open(root.clone(), Script::with(&[]));
    let after = recovery.send("steer-1", "steered", Delivery::Steer).await;
    assert_eq!(
        after, before,
        "the restarted daemon answers the same receipt"
    );
    let _ = std::fs::remove_dir_all(&root);
}
